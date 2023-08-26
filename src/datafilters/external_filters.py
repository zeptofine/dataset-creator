from __future__ import annotations

import os
from collections.abc import Callable, Iterable
from datetime import datetime
from enum import Enum
from typing import Annotated

import imagehash
import imagesize
from PIL import Image
from polars import DataFrame, Expr, List, col

from .base_filters import Column, Comparable, DataRule, FastComparable


def whash_db4(img) -> imagehash.ImageHash:
    return imagehash.whash(img, mode="db4")


class ResRule(DataRule, FastComparable):
    """A filter checking the size of an image."""

    config_keyword = "resolution"

    def __init__(
        self,
        min=0,
        max=2048,
        crop: Annotated[
            bool, "if true, then it will check if it is valid after cropping a little to be divisible by scale"
        ] = False,
        scale=4,
    ) -> None:
        super().__init__()
        self.schema = (Column(self, "resolution", List(int), col("path").apply(imagesize.get)),)
        self.min: int | None = min
        self.max: int | None = max
        self.crop: bool | None = crop
        self.scale: int | None = scale

    def fast_comp(self) -> Expr | bool:
        if self.crop:
            return col("resolution").apply(lambda lst: self.is_valid(map(self.resize, lst)))
        return col("resolution").apply(lambda lst: all(dim % self.scale == 0 for dim in lst) and self.is_valid(lst))

    def is_valid(self, lst: Iterable[int]) -> bool:
        lst = set(lst)
        return not ((self.min and min(lst) < self.min) or (self.max and max(lst) > self.max))

    def resize(self, i: int) -> int:
        return (i // self.scale) * self.scale  # type: ignore


def get_channels(pth: str) -> int:
    return len(Image.open(pth).getbands())


class ChannelRule(DataRule, FastComparable):
    """Checks the number of channels in the image."""

    config_keyword = "channels"

    def __init__(self, min_channels=1, max_channels=4) -> None:
        super().__init__()
        self.schema = (Column(self, "channels", int, col("path").apply(get_channels)),)
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


_RESOLVERS: dict[str, Expr | bool] = {
    "ignore_all": False,
    "newest": col("modifiedtime") == col("modifiedtime").max(),
    "oldest": col("modifiedtime") == col("modifiedtime").min(),
    "size": col("size") == col("size").max(),
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


class RESOLVERS(str, Enum):
    """
    Available resolvers.
    """

    IGNORE_ALL = "ignore_all"
    NEWEST = "newest"
    OLDEST = "oldest"
    SIZE = "size"


class HashRule(DataRule, Comparable):
    config_keyword = "hashing"

    def __init__(
        self,
        hasher: HASHERS = HASHERS.AVERAGE,
        resolver: RESOLVERS = RESOLVERS.IGNORE_ALL,
        get_optional_cols=False,
    ) -> None:
        super().__init__()
        self.schema = (Column(self, "hash", str, col("path").apply(self._hash_img)),)
        if get_optional_cols or resolver in [RESOLVERS.NEWEST, RESOLVERS.OLDEST]:
            self.schema = (
                *self.schema,
                Column(self, "modifiedtime", datetime),
            )
        if get_optional_cols or resolver == RESOLVERS.SIZE:
            self.schema = (*self.schema, Column(self, "size", int, col("path").apply(get_size)))
        self.hasher: Callable[[Image.Image], imagehash.ImageHash] = _HASHERS[hasher]
        self.resolver: Expr | bool = _RESOLVERS[resolver]

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

    def _hash_img(self, pth) -> str:
        assert self.hasher is not None
        return str(self.hasher(Image.open(pth)))
