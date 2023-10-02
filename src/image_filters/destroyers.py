import typing
from math import sqrt
from random import choice, randint, shuffle
from typing import Literal

import cv2
import ffmpeg
import numpy as np
from numpy import ndarray
from PIL import Image

from ..configs.configtypes import FilterData
from ..datarules.base_rules import Filter

np_gen = np.random.default_rng()


BlurAlgorithms = Literal["average", "gaussian", "isotropic", "anisotropic"]
AllBlurAlgos = typing.get_args(BlurAlgorithms)


class BlurData(FilterData):
    algorithms: list[str]
    blur_range: list[int]
    scale: float


class Blur(Filter):
    @staticmethod
    def run(
        img: np.ndarray,
        algorithms: list[BlurAlgorithms] | None = None,
        blur_range: tuple[int, int] = (1, 16),
        scale: float = 0.25,
    ) -> np.ndarray:
        if algorithms is None:
            algorithms = ["average"]

        algorithm: BlurAlgorithms = choice(algorithms)

        start, stop = blur_range
        if algorithm in ["average", "gaussian"]:
            ksize: int = int((randint(start, stop) | 1) * scale)
            ksize = ksize + (ksize % 2 == 0)  # ensure ksize is odd

            if algorithm == "average":
                return cv2.blur(img, (ksize, ksize))
            return cv2.GaussianBlur(img, (ksize, ksize), 0)

        if algorithm in ["isotropic", "anisotropic"]:
            sigma1: float = randint(start, stop) * scale
            ksize1: int = 2 * int(4 * sigma1 + 0.5) + 1
            if algorithm == "anisotropic":
                return cv2.GaussianBlur(img, (ksize1, ksize1), sigmaX=sigma1, sigmaY=sigma1)

            sigma2: float = randint(start, stop) * scale
            ksize2: int = 2 * int(4 * sigma2 + 0.5) + 1
            return cv2.GaussianBlur(img, (ksize1, ksize2), sigmaX=sigma1, sigmaY=sigma2)

        return img


NoiseAlgorithms = Literal["uniform", "gaussian", "color", "gray"]
AllNoiseAlgos = typing.get_args(NoiseAlgorithms)


class NoiseData(FilterData):
    algorithms: list[str]
    intensity_range: list[int]
    scale: float


class Noise(Filter):
    @staticmethod
    def run(
        img: ndarray,
        algorithms: list[NoiseAlgorithms] | None = None,
        intensity_range: tuple[int, int] = (1, 16),
        scale: float = 0.25,
    ) -> ndarray:
        if algorithms is None:
            algorithms = ["uniform"]
        algorithm = choice(algorithms)

        start, stop = intensity_range
        if algorithm == "uniform":
            intensity = randint(start, stop) * scale
            noise = np_gen.uniform(-intensity, intensity, img.shape)
            return cv2.add(img, noise.astype(img.dtype))

        if algorithm == "gaussian":
            sigma = sqrt(randint(start, stop) * scale)
            noise = np_gen.normal(0, sigma, img.shape)
            return cv2.add(img, noise.astype(img.dtype))

        if algorithm == "color":
            noise = np.zeros_like(img)
            s = (randint(start, stop), randint(start, stop), randint(start, stop))
            cv2.randn(noise, 0, s)  # type: ignore
            return img + noise

        # algorithm (should be) == NoiseAlgorithms.GRAY
        noise = np.zeros((img.shape[0], img.shape[1]), dtype=np.uint8)
        cv2.randn(noise, 0, (randint(start, stop),))  # type: ignore
        return img + noise
