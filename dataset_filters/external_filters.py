from __future__ import annotations
import os
from collections.abc import Callable, Collection, Iterable
from typing import Any

import imagehash
import imagesize
from polars import List, col
from PIL import Image
from polars import DataFrame, Expr

from .base_filters import Comparable, DataFilter, FastComparable


class ResFilter(DataFilter, FastComparable):
    """A filter checking the size of an image."""

    def __init__(self) -> None:
        super().__init__()
        self.column_schema = {"resolution": List(int)}
        self.build_schema = {"resolution": col("path").apply(imagesize.get)}
        self.config = (
            "resolution",
            {
                "enabled": True,
                "min": 0,
                "max": 2048,
                "crop": False,
                "!#crop": "if true, then it will check if it is valid after cropping a little to be divisible by scale",
                "scale": 4,
            },
        )

        self.min: int | None = 0
        self.max: int | None = 2048
        self.crop: bool | None = False
        self.scale: int | None = 4

    def populate_from_cfg(self, dct: dict[str, Any]):
        super().populate_from_cfg(dct)
        assert not ((self.min and self.min <= 0) or (self.max and self.max <= 0)), "selected min and/or max is invalid"

    def fast_comp(self) -> Expr | bool:
        if self.crop:
            return col("resolution").apply(lambda lst: self.is_valid(map(self.resize, lst)))
        return col("resolution").apply(lambda lst: all(dim % self.scale == 0 for dim in lst) and self.is_valid(lst))

    def is_valid(self, lst: Iterable[int]) -> bool:
        lst = set(lst)
        return not ((self.min and min(lst) < self.min) or (self.max and max(lst) > self.max))

    def resize(self, i: int) -> int:
        return (i // self.scale) * self.scale  # type: ignore


class ChannelFilter(DataFilter, FastComparable):
    """Checks the number of channels in the image."""

    def __init__(self) -> None:
        super().__init__()
        self.column_schema = {"channels": int}
        self.build_schema = {"channels": col("path").apply(self.get_channels)}
        self.config = (
            "channels",
            {
                "enabled": False,
                "channel_num": 3,
                "strict": False,
                "!#strict": " if true, only images with {channel_num} channels are available",
            },
        )

        self.channel_num: int | None = None
        self.strict: bool | None = None

    def get_channels(self, pth: str) -> int:
        return len(Image.open(pth).getbands())

    def fast_comp(self) -> Expr | bool:
        if not self.channel_num:
            return True
        if self.strict:
            return col("channels") == self.channel_num
        return col("channels") <= self.channel_num


class HashFilter(DataFilter, Comparable):
    def __init__(self) -> None:
        super().__init__()
        self.column_schema = {"hash": str}  # type: ignore
        self.build_schema: dict[str, Expr] = {"hash": col("path").apply(self._hash_img)}
        self.config = (
            "hashing",
            {
                "enabled": False,
                "hasher": "average",
                "!#hasher": f" {' | '.join(IMHASH_TYPES.keys())}",
                "resolver": "newest",
                "!#resolver": f" {' | '.join(IMHASH_RESOLVERS.keys())}",
            },
        )

        self.hasher: Callable[[Image.Image], imagehash.ImageHash] | None = None
        self.resolver: Callable[[], Expr | bool] | None = None

    def populate_from_cfg(self, dct: dict[str, Any]):
        assert "hasher" in dct and "resolver" in dct
        self.hasher = IMHASH_TYPES[dct["hasher"]]
        self.resolver = IMHASH_RESOLVERS[dct["resolver"]]

    def compare(self, lst: Collection, cols: DataFrame) -> set:
        assert self.resolver is not None
        applied: DataFrame = (
            cols.filter(col("hash").is_in(cols.filter(col("path").is_in(lst)).select(col("hash")).unique().to_series()))
            .groupby("hash")
            .apply(lambda df: df.filter(self.resolver()) if len(df) > 1 else df)  # type: ignore
        )

        resolved_paths = set(applied.select(col("path")).to_series())
        return resolved_paths

    def _hash_img(self, pth) -> str:
        assert self.hasher is not None
        return str(self.hasher(Image.open(pth)))

    @staticmethod
    def _ignore_all() -> Expr | bool:
        return False

    @staticmethod
    def _accept_newest() -> Expr:
        return col("modifiedtime") == col("modifiedtime").max()

    @staticmethod
    def _accept_oldest() -> Expr:
        return col("modifiedtime") == col("modifiedtime").min()

    @staticmethod
    def _accept_biggest() -> Expr:
        sizes: Expr = col("path").apply(lambda p: os.stat(str(p)).st_size)
        return sizes == sizes.max()


IMHASH_TYPES: dict[str, Callable] = {
    "average": imagehash.average_hash,
    "crop_resistant": imagehash.crop_resistant_hash,
    "color": imagehash.colorhash,
    "dhash": imagehash.dhash,
    "dhash_vertical": imagehash.dhash_vertical,
    "phash": imagehash.phash,
    "phash_simple": imagehash.phash_simple,
    "whash": imagehash.whash,
    "whash-db4": lambda img: imagehash.whash(img, mode="db4"),
}

IMHASH_RESOLVERS: dict[str, Callable] = {
    "ignore_all": HashFilter._ignore_all,
    "newest": HashFilter._accept_newest,
    "oldest": HashFilter._accept_oldest,
    "size": HashFilter._accept_biggest,
}
