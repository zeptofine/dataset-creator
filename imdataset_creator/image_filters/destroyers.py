import logging
import subprocess
import typing
from dataclasses import dataclass
from enum import Enum
from math import sqrt
from random import choice, randint
from typing import Literal, Self

import cv2
import ffmpeg
import numpy as np
from numpy import ndarray

from ..configs.configtypes import FilterData
from ..datarules import Filter
from ..enum_helpers import listostr2listoenum

log = logging.getLogger()

np_gen = np.random.default_rng()


class BlurAlgorithm(Enum):
    AVERAGE = 0
    GAUSSIAN = 1
    ISOTROPIC = 2
    ANISOTROPIC = 3


class BlurData(FilterData):
    algorithms: list[str]
    blur_range: list[int]
    scale: float


@dataclass(frozen=True)
class Blur(Filter):
    algorithms: list[BlurAlgorithm] | None = None
    blur_range: tuple[int, int] = (1, 16)
    scale: float = 0.25

    def run(
        self,
        img: np.ndarray,
    ) -> np.ndarray:
        algorithms = self.algorithms or [BlurAlgorithm.AVERAGE]
        algorithm: BlurAlgorithm = choice(algorithms)

        start, stop = self.blur_range
        ri = randint(start, stop)
        ksize: int
        if algorithm == BlurAlgorithm.AVERAGE:
            ksize = int(ri * self.scale)
            ksize = ksize + (ksize % 2 == 0)  # ensure ksize is odd
            return cv2.blur(img, (ksize, ksize))
        if algorithm == BlurAlgorithm.GAUSSIAN:
            ksize = int((ri | 1) * self.scale)
            ksize = ksize + (ksize % 2 == 0)  # ensure ksize is odd
            return cv2.GaussianBlur(img, (ksize, ksize), 0)

        if algorithm == BlurAlgorithm.ISOTROPIC or algorithm == BlurAlgorithm.ANISOTROPIC:
            sigma1: float = ri * self.scale
            ksize1: int = 2 * int(4 * sigma1 + 0.5) + 1
            if algorithm == BlurAlgorithm.ANISOTROPIC:
                return cv2.GaussianBlur(img, (ksize1, ksize1), sigmaX=sigma1, sigmaY=sigma1)

            sigma2: float = ri * self.scale
            ksize2: int = 2 * int(4 * sigma2 + 0.5) + 1
            return cv2.GaussianBlur(img, (ksize1, ksize2), sigmaX=sigma1, sigmaY=sigma2)

        return img

    @classmethod
    def from_cfg(cls, cfg: BlurData) -> Self:
        return cls(
            algorithms=listostr2listoenum(cfg["algorithms"], BlurAlgorithm),
            blur_range=cfg["blur_range"],  # type: ignore
            scale=cfg["scale"],
        )


class NoiseAlgorithm(Enum):
    UNIFORM = 0
    GAUSSIAN = 1
    COLOR = 2
    GRAY = 3


class NoiseData(FilterData):
    algorithms: list[str]
    intensity_range: list[int]
    scale: float


@dataclass(frozen=True)
class Noise(Filter):
    algorithms: list[NoiseAlgorithm] | None = None
    intensity_range: tuple[int, int] = (1, 16)
    scale: float = 0.25

    def run(self, img: ndarray) -> ndarray:
        algorithms = self.algorithms or [NoiseAlgorithm.UNIFORM]
        algorithm = choice(algorithms)

        if algorithm == NoiseAlgorithm.UNIFORM:
            intensity = randint(*self.intensity_range) * self.scale
            noise = np_gen.uniform(-intensity, intensity, img.shape)
            return cv2.add(img, noise.astype(img.dtype))

        if algorithm == NoiseAlgorithm.GAUSSIAN:
            sigma = sqrt(randint(*self.intensity_range) * self.scale)
            noise = np_gen.normal(0, sigma, img.shape)
            return cv2.add(img, noise.astype(img.dtype))

        if algorithm == NoiseAlgorithm.COLOR:
            noise = np.zeros_like(img)
            s = (randint(*self.intensity_range), randint(*self.intensity_range), randint(*self.intensity_range))
            cv2.randn(noise, 0, s)  # type: ignore
            return img + noise

        # algorithm (should be) == NoiseAlgorithms.GRAY
        noise = np.zeros((img.shape[0], img.shape[1]), dtype=np.uint8)
        cv2.randn(noise, 0, (randint(*self.intensity_range),))  # type: ignore
        noise = noise[..., None]
        return img + noise

    @classmethod
    def from_cfg(cls, cfg: NoiseData) -> Self:
        return cls(
            algorithms=listostr2listoenum(cfg["algorithms"], NoiseAlgorithm),
            intensity_range=cfg["intensity_range"],  # type: ignore
            scale=cfg["scale"],
        )


class CompressionAlgorithms(Enum):
    JPEG = "jpeg"
    WEBP = "webp"
    H264 = "h264"
    HEVC = "hevc"
    MPEG = "mpeg"
    MPEG2 = "mpeg2"


class CompressionData(FilterData):
    algorithms: list[str]
    jpeg_quality_range: list[int]
    webp_quality_range: list[int]
    h264_crf_range: list[int]
    hevc_crf_range: list[int]
    mpeg_bitrate: int
    mpeg2_bitrate: int


@dataclass(frozen=True, repr=False)
class Compression(Filter):
    algorithms: list[CompressionAlgorithms] | None = None
    jpeg_quality_range: tuple[int, int] = (0, 100)
    webp_quality_range: tuple[int, int] = (1, 100)
    h264_crf_range: tuple[int, int] = (20, 28)
    hevc_crf_range: tuple[int, int] = (25, 33)
    mpeg_bitrate: int = 4_500_000
    mpeg2_bitrate: int = 4_000_000

    def run(self, img: ndarray):
        algos = self.algorithms or [CompressionAlgorithms.JPEG]

        algorithm = choice(algos)
        quality: int
        enc_img: ndarray
        if algorithm == CompressionAlgorithms.JPEG:
            quality = randint(*self.jpeg_quality_range)
            enc_img = cv2.imencode(".jpg", img, [int(cv2.IMWRITE_JPEG_QUALITY), quality])[1]
            return cv2.imdecode(enc_img, 1)

        if algorithm == CompressionAlgorithms.WEBP:
            quality = randint(*self.webp_quality_range)
            enc_img = cv2.imencode(".webp", img, [int(cv2.IMWRITE_WEBP_QUALITY), quality])[1]
            return cv2.imdecode(enc_img, 1)

        if algorithm in [
            CompressionAlgorithms.H264,
            CompressionAlgorithms.HEVC,
            CompressionAlgorithms.MPEG,
            CompressionAlgorithms.MPEG2,
        ]:
            height, width, _ = img.shape
            codec = algorithm.value
            container = "mpeg"

            output_args: dict[str, int | str]
            crf: int
            if algorithm == CompressionAlgorithms.H264:
                crf = randint(*self.h264_crf_range)
                output_args = {"crf": crf}
            elif algorithm == CompressionAlgorithms.HEVC:
                crf = randint(*self.hevc_crf_range)
                output_args = {"crf": crf, "x265-params": "log-level=0"}
            elif algorithm == CompressionAlgorithms.MPEG:
                output_args = {"b": self.mpeg_bitrate}
                codec = "mpeg1video"

            else:
                output_args = {"b": self.mpeg2_bitrate}
                codec = "mpeg2video"

            compressor = (
                ffmpeg.input("pipe:", format="rawvideo", pix_fmt="bgr24", s=f"{width}x{height}")
                .output("pipe:", format=container, vcodec=codec, **output_args)
                .global_args("-loglevel", "error")
                .global_args("-max_muxing_queue_size", "300000")
                .run_async(pipe_stdin=True, pipe_stdout=True)
            )
            compressor.stdin.write(img.tobytes())
            compressor.stdin.close()
            reader = (
                ffmpeg.input("pipe:", format=container)
                .output("pipe:", format="rawvideo", pix_fmt="bgr24")
                .global_args("-loglevel", "error")
                .run_async(pipe_stdin=True, pipe_stdout=True)
            )
            out, _ = reader.communicate(input=compressor.stdout.read())

            try:
                compressor.wait(10)
                newimg = np.frombuffer(out, np.uint8)
                if len(newimg) != height * width * 3:
                    log.warning("New image size does not match")
                    newimg = newimg[: height * width * 3]  # idrk why i need this sometimes

                return newimg.reshape((height, width, 3))
            except subprocess.TimeoutExpired as e:
                compressor.send_signal("SIGINT")
                log.warning(f"{e}")

        return img

    @classmethod
    def from_cfg(cls, cfg: CompressionData) -> Self:
        return cls(
            algorithms=listostr2listoenum(cfg["algorithms"], CompressionAlgorithms),
            jpeg_quality_range=cfg["jpeg_quality_range"],  # type: ignore
            webp_quality_range=cfg["webp_quality_range"],  # type: ignore
            h264_crf_range=cfg["h264_crf_range"],  # type: ignore
            hevc_crf_range=cfg["hevc_crf_range"],  # type: ignore
            mpeg_bitrate=cfg["mpeg_bitrate"],
            mpeg2_bitrate=cfg["mpeg2_bitrate"],
        )
