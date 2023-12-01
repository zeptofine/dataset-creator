from __future__ import annotations

import os
from enum import Enum, StrEnum
from functools import cache
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, Self

import imagehash
import polars as pl
from PIL import Image
from polars import DataFrame, Expr, col

from ..configs.configtypes import SpecialItemData
from .base_rules import (
    DataColumn,
    DataFrameMatcher,
    ExprMatcher,
    Producer,
    ProducerSchema,
    Production,
    Rule,
    combine_expr_conds,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    import numpy as np


def whash_db4(img) -> imagehash.ImageHash:
    return imagehash.whash(img, mode="db4")


def _get_hwc(pth):
    img = Image.open(pth)
    return img.height, img.width, len(img.getbands())


def get_hwc(pth):
    h, w, c = _get_hwc(pth)
    return {
        "height": h,
        "width": w,
        "channels": c,
    }


def get_hwc_from_np(img: np.ndarray):
    h, w = img.shape[:2]
    c = 1 if img.ndim == 2 else img.shape[2]
    return h, w, c


class ImShapeProducer(Producer):
    produces = MappingProxyType(
        {
            "width": Production(int, 0),
            "height": Production(int, 0),
            "channels": Production(int, 0),
        }
    )

    def __call__(self) -> ProducerSchema:
        return [
            {
                "shape": col("path").apply(get_hwc),
            },
            {
                "width": col("shape").struct.field("width"),
                "height": col("shape").struct.field("height"),
                "channels": col("shape").struct.field("channels"),
            },
        ]


class ResData(SpecialItemData):
    min_res: int
    max_res: int
    crop: bool
    scale: int


class ResRule(Rule):
    """A filter checking the size of an image."""

    def __init__(
        self,
        min_res=0,
        max_res=2048,
        crop: bool = False,
        scale=4,
    ) -> None:
        super().__init__()
        self.requires = (
            DataColumn("width", int),
            DataColumn("height", int),
        )

        smallest = pl.min_horizontal(col("width"), col("height"))
        largest = pl.max_horizontal(col("width"), col("height"))

        exprs = []

        if min_res:
            exprs.append((smallest // scale * scale if crop else smallest) >= min_res)
        if max_res:
            exprs.append((largest // scale * scale if crop else largest) <= max_res)

        self.matcher = ExprMatcher(*exprs)

    @classmethod
    def get_cfg(cls) -> ResData:
        return {
            "min_res": 0,
            "max_res": 2048,
            "crop": False,
            "scale": 4,
        }

    @classmethod
    def from_cfg(cls, cfg: ResData) -> Self:
        return cls(
            min_res=cfg["min_res"],
            max_res=cfg["max_res"],
            crop=cfg["crop"],
            scale=cfg["scale"],
        )


class ChannelRule(Rule):
    """Checks the number of channels in the image."""

    def __init__(self, min_channels=1, max_channels=4) -> None:
        super().__init__()
        self.requires = DataColumn("channels", int)
        self.matcher = ExprMatcher(min_channels <= col("channels"), col("channels") <= max_channels)


def get_size(pth):
    return os.stat(pth).st_size


_HASHERS: dict[str, Callable] = {
    "average": imagehash.average_hash,
    "crop_resistant": imagehash.crop_resistant_hash,
    "color": imagehash.colorhash,
    "dhash": imagehash.dhash,
    "dhash_vertical": imagehash.dhash_vertical,
    "phash": imagehash.phash,
    "phash_simple": imagehash.phash_simple,
    "whash": imagehash.whash,
    "whash_db4": whash_db4,
}


class HASHERS(StrEnum):
    """
    Available hashers.
    """

    AVERAGE = "average"
    COLOR = "color"
    CROP_RESISTANT = "crop_resistant"
    DHASH = "dhash"
    DHASH_VERTICAL = "dhash_vertical"
    PHASH = "phash"
    PHASH_SIMPLE = "phash_simple"
    WHASH = "whash"
    WHASH_DB4 = "whash_db4"


class HashProducer(Producer):
    produces = MappingProxyType({"hash": Production(str, "87ffe7c991e08800")})

    def __init__(self, hash_type: HASHERS = HASHERS.AVERAGE):
        self.hasher: Callable[[Image.Image], imagehash.ImageHash] = _HASHERS[hash_type]

    def __call__(self) -> ProducerSchema:
        return [{"hash": col("path").apply(self._hash_img)}]

    def _hash_img(self, pth) -> str:
        return str(self.hasher(Image.open(pth)))


class HashRule(Rule):
    def __init__(self, resolver: str | Literal["ignore_all"] = "ignore_all") -> None:
        super().__init__()

        self.requires = DataColumn("hash", str)
        if resolver != "ignore_all":
            self.requires = (self.requires, DataColumn(resolver))
        self.resolver: Expr | bool = {"ignore_all": False}.get(resolver, col(resolver) == col(resolver).max())
        self.matcher = DataFrameMatcher(self.compare)

    def compare(self, partial: DataFrame, full: DataFrame) -> DataFrame:
        return partial.groupby("hash").apply(lambda group: group.filter(self.resolver) if len(group) > 1 else group)

    @classmethod
    def get_cfg(cls) -> dict:
        return {
            "resolver": "ignore_all",
        }
