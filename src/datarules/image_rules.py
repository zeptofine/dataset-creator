from __future__ import annotations

import os
from collections.abc import Callable, Iterable
from enum import Enum
from functools import cache
from types import MappingProxyType
from typing import Annotated, Literal

import imagehash
import polars as pl
from PIL import Image
from polars import DataFrame, Expr, col

from .base_rules import Column, Comparable, FastComparable, Producer, Rule


def whash_db4(img) -> imagehash.ImageHash:
    return imagehash.whash(img, mode="db4")


def get_channels(img: Image.Image) -> int:
    return len(img.getbands())


def imopen(pth):
    print(pth)
    return Image.open(pth)


@cache
def get_hwc(pth):
    img = Image.open(pth)
    return {"width": img.width, "height": img.height, "channels": len(img.getbands())}


class ImShapeProducer(Producer):
    produces = MappingProxyType({"width": int, "height": int, "channels": int})

    def __call__(self):
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


class ResRule(Rule, FastComparable):
    """A filter checking the size of an image."""

    config_keyword = "resolution"

    def __init__(
        self,
        min=0,
        max=2048,
        crop: Annotated[bool, "checks if valid after a slight crop"] = False,
        scale=4,
    ) -> None:
        super().__init__()
        self.requires = (
            Column(self, "width", int),
            Column(self, "height", int),
        )
        self.min: int | None = min
        self.max: int | None = max
        self.crop: bool | None = crop
        self.scale: int | None = scale

    def fast_comp(self) -> Expr | bool:
        if self.crop:
            return (pl.min_horizontal(col("width"), col("height")) // self.scale * self.scale >= self.min) & (
                pl.max_horizontal(col("width"), col("height")) // self.scale * self.scale <= self.max
            )
        return (pl.min_horizontal(col("width"), col("height")) >= self.min) & (
            pl.max_horizontal(col("width"), col("height")) <= self.max
        )

    def is_valid(self, lst: Iterable[int]) -> bool:
        lst = set(lst)
        return not ((self.min and min(lst) < self.min) or (self.max and max(lst) > self.max))

    def resize(self, i: int) -> int:
        return (i // self.scale) * self.scale  # type: ignore


class ChannelRule(Rule, FastComparable):
    """Checks the number of channels in the image."""

    config_keyword = "channels"

    def __init__(self, min_channels=1, max_channels=4) -> None:
        super().__init__()
        self.requires = Column(self, "channels", int)
        self.min_channels = min_channels
        self.max_channels = max_channels

    def fast_comp(self) -> Expr | bool:
        return (self.min_channels <= col("channels")) & (col("channels") <= self.max_channels)


def get_size(pth):
    return os.stat(pth).st_size  # noqa: PTH116


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


class HASHERS(str, Enum):
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
    produces = MappingProxyType({"hash": str})

    def __init__(self, hasher: HASHERS = HASHERS.AVERAGE):
        self.hasher: Callable[[Image.Image], imagehash.ImageHash] = _HASHERS[hasher]

    def __call__(self):
        return [{"hash": col("path").apply(self._hash_img)}]

    def _hash_img(self, pth) -> str:
        assert self.hasher is not None
        return str(self.hasher(Image.open(pth)))


class HashRule(Rule, Comparable):
    config_keyword = "hashing"

    def __init__(
        self,
        resolver: str | Literal["ignore_all"] = "ignore_all",
    ) -> None:
        super().__init__()

        self.requires = Column(self, "hash", str)
        if resolver != "ignore_all":
            self.requires = (self.requires, Column(self, resolver))
        self.resolver: Expr | bool = {"ignore_all": False}.get(resolver, col(resolver) == col(resolver).max())

    def compare(self, partial: DataFrame, full: DataFrame) -> DataFrame:
        return (
            full.filter(
                col("hash").is_in(
                    full.filter(col("path").is_in(partial.get_column("path"))).get_column("hash").unique()
                ),
            )
            .groupby("hash")
            .apply(lambda df: df.filter(self.resolver) if len(df) > 1 else df)  # type: ignore
        )

    @classmethod
    def get_cfg(cls) -> dict:
        return {
            "resolver": "ignore_all",
            "!#resolver": " ignore_all | column name",
            "hasher": "average",
            "!#hasher": " | ".join(HASHERS),
        }
