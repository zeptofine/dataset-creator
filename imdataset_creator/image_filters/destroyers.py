import logging
import subprocess
import typing
from math import sqrt
from random import choice, randint
from typing import Literal

import cv2
import ffmpeg
import numpy as np
from numpy import ndarray

from ..configs.configtypes import FilterData
from ..datarules.base_rules import Filter

log = logging.getLogger()

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

        if algorithm == "uniform":
            intensity = randint(*intensity_range) * scale
            noise = np_gen.uniform(-intensity, intensity, img.shape)
            return cv2.add(img, noise.astype(img.dtype))

        if algorithm == "gaussian":
            sigma = sqrt(randint(*intensity_range) * scale)
            noise = np_gen.normal(0, sigma, img.shape)
            return cv2.add(img, noise.astype(img.dtype))

        if algorithm == "color":
            noise = np.zeros_like(img)
            s = (randint(*intensity_range), randint(*intensity_range), randint(*intensity_range))
            cv2.randn(noise, 0, s)  # type: ignore
            return img + noise

        # algorithm (should be) == NoiseAlgorithms.GRAY
        noise = np.zeros((img.shape[0], img.shape[1]), dtype=np.uint8)
        cv2.randn(noise, 0, (randint(*intensity_range),))  # type: ignore
        noise = noise[..., None]
        return img + noise


CompressionAlgorithms = Literal[
    "jpeg",
    "webp",
    "h264",
    "hevc",
    "mpeg",
    "mpeg2",
]
AllCompressionAlgos: tuple[str, ...] = typing.get_args(CompressionAlgorithms)


class CompressionData(FilterData):
    algorithms: list[str]
    jpeg_quality_range: list[int]
    webp_quality_range: list[int]
    h264_crf_range: list[int]
    hevc_crf_range: list[int]
    mpeg_bitrate: int
    mpeg2_bitrate: int


class Compression(Filter):
    @staticmethod
    def run(
        img: ndarray,
        algorithms: list[CompressionAlgorithms] | None = None,
        jpeg_quality_range: tuple[int, int] = (0, 100),
        webp_quality_range: tuple[int, int] = (1, 100),
        h264_crf_range: tuple[int, int] = (20, 28),
        hevc_crf_range: tuple[int, int] = (25, 33),
        mpeg_bitrate: int = 4_500_000,
        mpeg2_bitrate: int = 4_000_000,
    ):
        if algorithms is None or not algorithms:
            algorithms = ["jpeg"]

        algorithm = choice(algorithms)
        quality: int
        encimg: ndarray
        if algorithm == "jpeg":
            quality = randint(*jpeg_quality_range)
            encimg = cv2.imencode(".jpg", img, [int(cv2.IMWRITE_JPEG_QUALITY), quality])[1]
            return cv2.imdecode(encimg, 1)

        if algorithm == "webp":
            quality = randint(*webp_quality_range)
            encimg = cv2.imencode(".webp", img, [int(cv2.IMWRITE_WEBP_QUALITY), quality])[1]
            return cv2.imdecode(encimg, 1)

        if algorithm in ["h264", "hevc", "mpeg", "mpeg2"]:
            height, width, _ = img.shape
            codec = algorithm
            container = "mpeg"

            output_args: dict[str, int | str]
            crf: int
            if algorithm == "h264":
                crf = randint(*h264_crf_range)
                output_args = {"crf": crf}
            elif algorithm == "hevc":
                crf = randint(*hevc_crf_range)
                output_args = {"crf": crf, "x265-params": "log-level=0"}
            elif algorithm == "mpeg":
                output_args = {"b": mpeg_bitrate}
                codec = "mpeg1video"

            else:
                output_args = {"b": mpeg2_bitrate}
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
            except subprocess.TimeoutExpired as e:
                compressor.send_signal("SIGINT")
                log.warning(f"{e}")
            newimg = np.frombuffer(out, np.uint8)
            if len(newimg) != height * width * 3:
                log.warning("New image size does not match")
                newimg = newimg[: height * width * 3]  # idrk why i need this sometimes

            return newimg.reshape((height, width, 3))

        return img
